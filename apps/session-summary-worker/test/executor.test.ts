import { createHash } from "node:crypto";

import type { ParticipantTaskExecutorContext } from "@dungle-scrubs/tether-client";
import type {
  SessionEvent,
  SessionSummaryCandidateSubmission,
  SessionSummaryGenerationJob,
  SessionSummaryOllamaIdentity,
  TaskRecord,
} from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import { createSessionSummaryExecutor } from "../src/executor.js";
import { OllamaClient } from "../src/ollama.js";
import { TetherApiClient } from "../src/tether-api.js";

const ollamaIdentity: SessionSummaryOllamaIdentity = {
  contextSize: 32_768,
  model: "evaluated-model",
  quantization: "Q4_K_M",
  revision: `sha256:${"a".repeat(64)}`,
  thinkingMode: "disabled",
};

const events: readonly SessionEvent[] = [
  {
    createdAt: "2026-07-17T00:00:00.000Z",
    eventId: "event-10",
    payload: { decision: "keep exact range" },
    producerId: "producer-1",
    seq: 10,
    sessionId: "session-1",
    type: "user.message",
  },
  {
    createdAt: "2026-07-17T00:00:01.000Z",
    eventId: "event-11",
    payload: { status: "approved" },
    producerId: "producer-2",
    seq: 11,
    sessionId: "session-1",
    type: "approval.recorded",
  },
];

const source = {
  eventCount: events.length,
  firstEventId: events[0]?.eventId ?? "",
  lastEventId: events.at(-1)?.eventId ?? "",
  rangeHash: createHash("sha256")
    .update(events.map((event) => `${event.seq}:${event.eventId}\n`).join(""))
    .digest("hex"),
};

const job: SessionSummaryGenerationJob = {
  budgetClass: "standard",
  deadlineAt: "2099-07-17T00:00:00.000Z",
  expectedPrevious: { coversSeqTo: null, summaryId: null },
  inputLimitBytes: 64_000,
  kind: "session_summary.generate.v1",
  ollama: ollamaIdentity,
  outputLimitBytes: 64_000,
  outputSchemaVersion: "session-summary.v1",
  previousSummary: null,
  producer: { id: "session-summary-worker", version: "1" },
  promptVersion: "session-summary.v1",
  range: { from: 10, to: 11 },
  sessionId: "session-1",
  source,
  summaryId: "summary-1",
  taskId: "task-1",
};

const task: TaskRecord = {
  cancelledAt: null,
  claimExpiredAt: null,
  claimExpiredBy: null,
  claimExpiresAt: "2099-07-17T00:00:00.000Z",
  claimId: "claim_worker_1",
  claimedAt: "2026-07-17T00:00:00.000Z",
  claimedBy: "worker-1",
  completedAt: null,
  createdAt: "2026-07-17T00:00:00.000Z",
  failedAt: null,
  failure: null,
  input: { ...job },
  kind: "session_summary_generation",
  objective: "Generate a bounded Session Summary",
  releasedAt: null,
  releasedBy: null,
  result: null,
  sessionId: "session-1",
  taskId: "task-1",
};

describe("session summary executor", () => {
  it("submits a validated candidate for the exact selected range", async () => {
    let ollamaEvents: readonly SessionEvent[] = [];
    let submission: SessionSummaryCandidateSubmission | null = null;
    const fetch: typeof globalThis.fetch = async (input, init) => {
      const url = new URL(
        typeof input === "string" ? input : input instanceof URL ? input : input.url,
      );
      if (url.origin === "http://tether.test" && init?.method !== "POST") {
        expect(url.searchParams.get("after")).toBe("9");
        expect(url.searchParams.get("limit")).toBe("2");
        return Response.json({
          events,
          pagination: {
            afterSeq: 9,
            hasMore: false,
            limit: 2,
            nextAfterSeq: 11,
            returned: 2,
          },
        });
      }
      if (url.origin === "http://ollama.test") {
        const request = JSON.parse(String(init?.body)) as {
          readonly messages: readonly { readonly content: string; readonly role: string }[];
        };
        const prompt = request.messages.find((message) => message.role === "user")?.content ?? "";
        ollamaEvents = (JSON.parse(prompt) as { readonly events: readonly SessionEvent[] }).events;
        return Response.json({
          message: {
            content: JSON.stringify({
              facts: [
                {
                  category: "decision",
                  sourceEventIds: ["event-10"],
                  statement: "The exact range is retained.",
                  subjectIds: [],
                },
              ],
              headline: "Exact range",
              narrative: "The selected events were summarized.",
              openQuestions: [],
            }),
          },
        });
      }
      submission = JSON.parse(String(init?.body)) as SessionSummaryCandidateSubmission;
      return Response.json({ status: "published" }, { status: 201 });
    };
    const executor = createSessionSummaryExecutor({
      evaluatedSelection: {
        candidate: {
          identity: ollamaIdentity,
          outputSchemaVersion: "session-summary.v1",
          promptVersion: "session-summary.v1",
        },
        status: "enabled",
      },
      maxExecutionMs: 60_000,
      ollama: new OllamaClient({
        baseUrl: "http://ollama.test",
        fetch,
        maxAttempts: 1,
        requestTimeoutMs: 1_000,
        retryBackoffMs: 1,
      }),
      tether: new TetherApiClient({
        authToken: "secret-token",
        baseUrl: "http://tether.test",
        fetch,
        pageSize: 10,
        requestTimeoutMs: 1_000,
      }),
    });

    const result = await executor({
      controlEpoch: 7,
      instanceId: "instance-1",
      participantId: "worker-1",
      publishOutput: async () => undefined,
      publishProgress: async () => undefined,
      recentEvents: [],
      sessionId: "session-1",
      signal: new AbortController().signal,
      task,
    } satisfies ParticipantTaskExecutorContext);

    expect(ollamaEvents).toEqual(events);
    expect(submission).toMatchObject({
      claimantId: "worker-1",
      controlEpoch: 7,
      instanceId: "instance-1",
      ollama: ollamaIdentity,
      range: job.range,
      sessionId: job.sessionId,
      source,
      summaryId: job.summaryId,
      taskId: job.taskId,
    });
    expect(result.result).toEqual({ status: "candidate_submitted" });
  });

  it("turns terminal invalid output into a bounded poison failure", async () => {
    let submissions = 0;
    const executor = createExecutorForFetch(async (input, init) => {
      const url = requestUrl(input);
      if (url.origin === "http://tether.test" && init?.method !== "POST") {
        return eventPage(events);
      }
      if (url.origin === "http://ollama.test") {
        return Response.json({ message: { content: "{" } });
      }
      submissions += 1;
      return Response.json({ status: "published" }, { status: 201 });
    });

    await expect(executor(createContext())).rejects.toMatchObject({
      code: "poison_range",
      failure: {
        attempt: 1,
        code: "poison_range",
        retryable: false,
      },
    });
    expect(submissions).toBe(0);
  });

  it("rejects candidate submission failure instead of returning task success", async () => {
    let submissions = 0;
    const executor = createExecutorForFetch(async (input, init) => {
      const url = requestUrl(input);
      if (url.origin === "http://tether.test" && init?.method !== "POST") {
        return eventPage(events);
      }
      if (url.origin === "http://ollama.test") {
        return validOllamaResponse();
      }
      submissions += 1;
      return Response.json({ reason: "stale_job" }, { status: 409 });
    });

    await expect(executor(createContext())).rejects.toMatchObject({
      code: "publication_conflict",
    });
    expect(submissions).toBe(1);
  });

  it("rejects oversized input before calling Ollama", async () => {
    let ollamaCalls = 0;
    const executor = createExecutorForFetch(async (input, init) => {
      const url = requestUrl(input);
      if (url.origin === "http://tether.test" && init?.method !== "POST") {
        return eventPage(events);
      }
      ollamaCalls += 1;
      return validOllamaResponse();
    });
    const oversizedJob = { ...job, inputLimitBytes: 1 };

    await expect(
      executor(createContext({ task: { ...task, input: oversizedJob } })),
    ).rejects.toMatchObject({ code: "poison_range" });
    expect(ollamaCalls).toBe(0);
  });

  it("rejects a missing control epoch before network work", async () => {
    let calls = 0;
    const executor = createExecutorForFetch(async () => {
      calls += 1;
      return eventPage(events);
    });

    await expect(executor(createContext({ controlEpoch: undefined }))).rejects.toMatchObject({
      code: "identity_mismatch",
    });
    expect(calls).toBe(0);
  });

  it("rejects a source identity mismatch before calling Ollama", async () => {
    let ollamaCalls = 0;
    const mismatched = [{ ...events[0], eventId: "wrong-event" }, events[1]].filter(
      (event): event is SessionEvent => event !== undefined,
    );
    const executor = createExecutorForFetch(async (input, init) => {
      const url = requestUrl(input);
      if (url.origin === "http://tether.test" && init?.method !== "POST") {
        return eventPage(mismatched);
      }
      ollamaCalls += 1;
      return validOllamaResponse();
    });

    await expect(executor(createContext())).rejects.toMatchObject({ code: "unsafe_sequence" });
    expect(ollamaCalls).toBe(0);
  });
});

function createExecutorForFetch(fetch: typeof globalThis.fetch) {
  return createSessionSummaryExecutor({
    evaluatedSelection: {
      candidate: {
        identity: ollamaIdentity,
        outputSchemaVersion: "session-summary.v1",
        promptVersion: "session-summary.v1",
      },
      status: "enabled",
    },
    maxExecutionMs: 60_000,
    ollama: new OllamaClient({
      baseUrl: "http://ollama.test",
      fetch,
      maxAttempts: 1,
      requestTimeoutMs: 1_000,
      retryBackoffMs: 1,
    }),
    tether: new TetherApiClient({
      authToken: "secret-token",
      baseUrl: "http://tether.test",
      fetch,
      pageSize: 10,
      requestTimeoutMs: 1_000,
    }),
  });
}

function createContext(
  overrides: { readonly controlEpoch?: number | undefined; readonly task?: TaskRecord } = {},
): ParticipantTaskExecutorContext {
  return {
    ...(overrides.controlEpoch === undefined && "controlEpoch" in overrides
      ? {}
      : { controlEpoch: overrides.controlEpoch ?? 7 }),
    instanceId: "instance-1",
    participantId: "worker-1",
    publishOutput: async () => undefined,
    publishProgress: async () => undefined,
    recentEvents: [],
    sessionId: "session-1",
    signal: new AbortController().signal,
    task: overrides.task ?? task,
  };
}

function requestUrl(input: string | URL | Request): URL {
  return new URL(typeof input === "string" ? input : input instanceof URL ? input : input.url);
}

function eventPage(pageEvents: readonly SessionEvent[]): Response {
  return Response.json({
    events: pageEvents,
    pagination: {
      afterSeq: 9,
      hasMore: false,
      limit: 2,
      nextAfterSeq: 11,
      returned: pageEvents.length,
    },
  });
}

function validOllamaResponse(): Response {
  return Response.json({
    message: {
      content: JSON.stringify({
        facts: [],
        headline: "Exact range",
        narrative: "The selected events were summarized.",
        openQuestions: [],
      }),
    },
  });
}
