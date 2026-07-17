import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";

import type { SessionSummaryCandidateSubmission } from "@dungle-scrubs/tether-protocol";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type { AuthContext } from "../src/auth/token.js";
import { handleSessionSummaryHttpRoute } from "../src/http-session-summary-route-handlers.js";
import type { ResourceLimits } from "../src/resource-limits.js";
import {
  computeSessionSummaryIntegrityHash,
  type SessionSummaryStore,
  SessionSummaryStoreError,
} from "../src/session-summary-store.js";

describe("Session Summary candidate route", () => {
  it("binds authenticated identity and runs insertion, validation, then publication", async () => {
    const submission = candidateSubmission();
    const calls: string[] = [];
    const inserted: SessionSummaryCandidateSubmission[] = [];
    const store = {
      submitCandidate: async (input: SessionSummaryCandidateSubmission) => {
        calls.push("submit");
        inserted.push(input);
        return {
          status: "published" as const,
          summaryId: input.summaryId,
          supersededSummaryId: null,
        };
      },
      insertCandidate: async (input: SessionSummaryCandidateSubmission) => {
        calls.push("insert");
        inserted.push(input);
        return { status: "inserted" as const, summaryId: input.summaryId };
      },
      publishCandidate: async (summaryId: string) => {
        calls.push("publish");
        return {
          status: "published" as const,
          summaryId,
          supersededSummaryId: null,
        };
      },
      validateCandidate: async (summaryId: string) => {
        calls.push("validate");
        return { status: "validated" as const, summaryId };
      },
    } as unknown as SessionSummaryStore;
    const response = new CapturingResponse();

    const handled = await Effect.runPromise(
      handleSessionSummaryHttpRoute({
        authContext: authContext("part_worker_1"),
        request: jsonRequest(submission),
        resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
        response: response as unknown as ServerResponse,
        store,
        url: new URL("http://localhost/sessions/sess_1/tasks/task_summary_1/summary-candidate"),
      }),
    );

    expect(handled).toBe(true);
    expect(calls).toEqual(["submit"]);
    expect(inserted[0]?.claimantId).toBe("part_worker_1");
    expect(response.statusCode).toBe(201);
    expect(response.body).toMatchObject({ status: "published", summaryId: "summary_1" });
  });

  it("rejects an unauthorized observer before any lifecycle mutation", async () => {
    const calls: string[] = [];
    const response = new CapturingResponse();

    const handled = await Effect.runPromise(
      handleSessionSummaryHttpRoute({
        authContext: { ...authContext("part_worker_1"), role: "observer" },
        request: jsonRequest(candidateSubmission()),
        resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
        response: response as unknown as ServerResponse,
        store: recordingStore(calls),
        url: new URL("http://localhost/sessions/sess_1/tasks/task_summary_1/summary-candidate"),
      }),
    );

    expect(handled).toBe(true);
    expect(calls).toEqual([]);
    expect(response.statusCode).toBe(403);
  });

  it("does not treat generic task completion as summary publication", async () => {
    const calls: string[] = [];
    const response = new CapturingResponse();

    const handled = await Effect.runPromise(
      handleSessionSummaryHttpRoute({
        authContext: authContext("part_worker_1"),
        request: jsonRequest({ result: { summary: "untrusted summary text" } }),
        resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
        response: response as unknown as ServerResponse,
        store: recordingStore(calls),
        url: new URL("http://localhost/sessions/sess_1/tasks/task_summary_1/complete"),
      }),
    );

    expect(handled).toBe(false);
    expect(calls).toEqual([]);
    expect(response.statusCode).toBeNull();
  });

  it("returns a stable client error for an expected candidate rejection", async () => {
    const response = new CapturingResponse();
    const store = {
      ...recordingStore([]),
      submitCandidate: async () => {
        throw new SessionSummaryStoreError("integrity_mismatch", "internal integrity detail");
      },
    } satisfies SessionSummaryStore;

    const handled = await Effect.runPromise(
      handleSessionSummaryHttpRoute({
        authContext: authContext("part_worker_1"),
        request: jsonRequest(candidateSubmission()),
        resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
        response: response as unknown as ServerResponse,
        store,
        url: new URL("http://localhost/sessions/sess_1/tasks/task_summary_1/summary-candidate"),
      }),
    );

    expect(handled).toBe(true);
    expect(response.statusCode).toBe(422);
    expect(response.body).toEqual({
      error: "Session Summary candidate rejected",
      reason: "integrity_mismatch",
    });
  });
});

function recordingStore(calls: string[]): SessionSummaryStore {
  return {
    insertCandidate: async (input) => {
      calls.push("insert");
      return { status: "inserted", summaryId: input.summaryId };
    },
    inspectCandidate: async () => null,
    publishCandidate: async (summaryId) => {
      calls.push("publish");
      return { status: "published", summaryId, supersededSummaryId: null };
    },
    quarantineCandidate: async (input) => ({
      status: "quarantined",
      summaryId: input.summaryId,
    }),
    readLatestPublished: async () => null,
    selectAndReserveGeneration: async () => ({ status: "caught_up" }),
    submitCandidate: async (input) => {
      calls.push("submit");
      return {
        status: "published",
        summaryId: input.summaryId,
        supersededSummaryId: null,
      };
    },
    validateCandidate: async (summaryId) => {
      calls.push("validate");
      return { status: "validated", summaryId };
    },
  };
}

class CapturingResponse {
  statusCode: number | null = null;
  body: Record<string, unknown> | null = null;

  writeHead(statusCode: number): this {
    this.statusCode = statusCode;
    return this;
  }

  end(payload?: string): void {
    if (payload !== undefined) {
      this.body = JSON.parse(payload) as Record<string, unknown>;
    }
  }
}

function authContext(participantId: string): AuthContext {
  return {
    expiresAt: "2099-01-01T00:00:00.000Z",
    kid: "default",
    participantId,
    role: "participant",
    sessionScope: "sess_1",
  };
}

function jsonRequest(body: unknown): IncomingMessage {
  const request = Readable.from([Buffer.from(JSON.stringify(body))]) as unknown as IncomingMessage;
  (request as { method?: string }).method = "POST";
  return request;
}

function candidateSubmission(): SessionSummaryCandidateSubmission {
  const content = {
    facts: [],
    headline: "Bounded history",
    narrative: "A compact source-grounded session history.",
    openQuestions: [],
  };
  return {
    claimantId: "part_worker_1",
    content,
    controlEpoch: 4,
    instanceId: "inst_worker_1",
    integrity: { algorithm: "sha256", hash: computeSessionSummaryIntegrityHash(content) },
    kind: "session_summary.candidate.v1",
    ollama: {
      contextSize: 32_768,
      model: "qwen3:8b",
      quantization: "Q4_K_M",
      revision: "sha256:model-revision",
      thinkingMode: "enabled",
    },
    range: { from: 1, to: 20 },
    sessionId: "sess_1",
    source: {
      eventCount: 20,
      firstEventId: "evt_1",
      lastEventId: "evt_20",
      rangeHash: "b".repeat(64),
    },
    summaryId: "summary_1",
    taskId: "task_summary_1",
  };
}
