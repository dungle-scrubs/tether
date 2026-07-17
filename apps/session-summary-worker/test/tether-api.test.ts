import type {
  SessionEvent,
  SessionSummaryCandidateSubmission,
} from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import { TetherApiClient } from "../src/tether-api.js";

describe("TetherApiClient", () => {
  it("pages an exact range with bearer authentication", async () => {
    const afterValues: string[] = [];
    const authorizations: string[] = [];
    const client = createClient(async (input, init) => {
      const url = new URL(String(input));
      afterValues.push(url.searchParams.get("after") ?? "");
      authorizations.push(new Headers(init?.headers).get("authorization") ?? "");
      const after = Number(url.searchParams.get("after"));
      return pageResponse([event(after + 1)], after + 1 < 11);
    });

    await expect(
      client.fetchExactRange({
        from: 10,
        sessionId: "session-1",
        signal: new AbortController().signal,
        to: 11,
      }),
    ).resolves.toEqual([event(10), event(11)]);
    expect(afterValues).toEqual(["9", "10"]);
    expect(authorizations).toEqual(["Bearer secret-token", "Bearer secret-token"]);
  });

  it("rejects a non-contiguous range page", async () => {
    const client = createClient(async () => pageResponse([event(11)], false));

    await expect(
      client.fetchExactRange({
        from: 10,
        sessionId: "session-1",
        signal: new AbortController().signal,
        to: 11,
      }),
    ).rejects.toMatchObject({ code: "unsafe_sequence" });
  });

  it("maps request timeout to a bounded typed transport failure", async () => {
    const client = new TetherApiClient({
      authToken: "secret-token",
      baseUrl: "http://tether.test",
      fetch: async (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), {
            once: true,
          });
        }),
      pageSize: 1,
      requestTimeoutMs: 1,
    });

    await expect(
      client.fetchExactRange({
        from: 10,
        sessionId: "session-1",
        signal: new AbortController().signal,
        to: 11,
      }),
    ).rejects.toMatchObject({ code: "generation_unavailable", retryable: true });
  });

  it("maps a candidate submission conflict response to a non-retryable publication conflict", async () => {
    const client = createClient(async () =>
      Response.json({ error: "rejected", reason: "duplicate_submission" }, { status: 409 }),
    );

    await expect(
      client.submitCandidate(candidateSubmission(), new AbortController().signal),
    ).rejects.toMatchObject({ code: "publication_conflict", retryable: false });
  });

  it("maps candidate submission throttling and 5xx responses to retryable unavailability", async () => {
    for (const status of [429, 503]) {
      const client = createClient(async () => new Response(null, { status }));

      await expect(
        client.submitCandidate(candidateSubmission(), new AbortController().signal),
      ).rejects.toMatchObject({ code: "generation_unavailable", retryable: true });
    }
  });
});

function candidateSubmission(): SessionSummaryCandidateSubmission {
  return {
    claimantId: "part_worker_1",
    content: {
      facts: [],
      headline: "Bounded history",
      narrative: "A compact source-grounded session history.",
      openQuestions: [],
    },
    controlEpoch: 4,
    instanceId: "inst_worker_1",
    integrity: { algorithm: "sha256", hash: "a".repeat(64) },
    kind: "session_summary.candidate.v1",
    ollama: {
      contextSize: 32_768,
      model: "evaluated-model",
      quantization: "Q4_K_M",
      revision: `sha256:${"b".repeat(64)}`,
      thinkingMode: "disabled",
    },
    range: { from: 10, to: 11 },
    sessionId: "session-1",
    source: {
      eventCount: 2,
      firstEventId: "event-10",
      lastEventId: "event-11",
      rangeHash: "c".repeat(64),
    },
    summaryId: "summary-1",
    taskId: "task-1",
  };
}

function createClient(fetch: typeof globalThis.fetch): TetherApiClient {
  return new TetherApiClient({
    authToken: "secret-token",
    baseUrl: "http://tether.test",
    fetch,
    pageSize: 1,
    requestTimeoutMs: 1_000,
  });
}

function event(seq: number): SessionEvent {
  return {
    createdAt: "2026-07-17T00:00:00.000Z",
    eventId: `event-${seq}`,
    payload: {},
    producerId: "producer",
    seq,
    sessionId: "session-1",
    type: "user.message",
  };
}

function pageResponse(events: readonly SessionEvent[], hasMore: boolean): Response {
  const afterSeq = (events[0]?.seq ?? 1) - 1;
  return Response.json({
    events,
    pagination: {
      afterSeq,
      hasMore,
      limit: 1,
      nextAfterSeq: events.at(-1)?.seq ?? afterSeq,
      returned: events.length,
    },
  });
}
