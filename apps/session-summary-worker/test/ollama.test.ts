import type { SessionSummaryGenerationJob } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import type { SessionSummaryWorkerError } from "../src/errors.js";
import { OllamaClient } from "../src/ollama.js";

const validContent = {
  facts: [],
  headline: "Bounded summary",
  narrative: "Only selected events are represented.",
  openQuestions: [],
} as const;

describe("OllamaClient", () => {
  it("retries classified transient responses within a finite attempt budget", async () => {
    let attempts = 0;
    const client = createClient(async () => {
      attempts += 1;
      return attempts < 3
        ? new Response(null, { status: 503 })
        : ollamaResponse(JSON.stringify(validContent));
    }, 3);

    await expect(generate(client)).resolves.toEqual(validContent);
    expect(attempts).toBe(3);
  });

  it("exhausts connection retries with a bounded typed failure", async () => {
    let attempts = 0;
    const client = createClient(async () => {
      attempts += 1;
      throw new TypeError("connection refused with sensitive endpoint details");
    }, 2);

    await expect(generate(client)).rejects.toMatchObject({
      code: "generation_unavailable",
      retryable: true,
    } satisfies Partial<SessionSummaryWorkerError>);
    expect(attempts).toBe(2);
  });

  it.each([
    ["malformed JSON", "{"],
    ["schema-invalid JSON", JSON.stringify({ headline: "missing fields" })],
  ])("rejects %s without retry", async (_label, content) => {
    let attempts = 0;
    const client = createClient(async () => {
      attempts += 1;
      return ollamaResponse(content);
    }, 3);

    await expect(generate(client)).rejects.toMatchObject({ code: "invalid_output" });
    expect(attempts).toBe(1);
  });

  it("rejects output above the job byte limit before parsing", async () => {
    const client = createClient(async () => ollamaResponse(JSON.stringify(validContent)), 1);

    await expect(generate(client, { outputLimitBytes: 4 })).rejects.toMatchObject({
      code: "invalid_output",
    });
  });

  it("cancels a streamed response after its raw envelope byte cap", async () => {
    let cancelled = false;
    const oversizedChunk = new Uint8Array(5_000);
    const client = createClient(
      async () =>
        new Response(
          new ReadableStream<Uint8Array>({
            cancel: () => {
              cancelled = true;
            },
            start: (controller) => {
              controller.enqueue(oversizedChunk);
            },
          }),
          { status: 200 },
        ),
      1,
    );

    await expect(generate(client, { outputLimitBytes: 1 })).rejects.toMatchObject({
      code: "invalid_output",
    });
    expect(cancelled).toBe(true);
  });

  it("propagates participant cancellation into the active Ollama request", async () => {
    const controller = new AbortController();
    let observedAbort = false;
    const client = createClient(
      async (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => {
              observedAbort = true;
              reject(init.signal?.reason);
            },
            { once: true },
          );
        }),
      3,
    );

    const generation = generate(client, { signal: controller.signal });
    controller.abort();

    await expect(generation).rejects.toMatchObject({ code: "cancelled" });
    expect(observedAbort).toBe(true);
  });

  it("bounds request timeouts and retry exhaustion", async () => {
    let attempts = 0;
    const client = new OllamaClient({
      baseUrl: "http://ollama.test",
      fetch: async (_input, init) => {
        attempts += 1;
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), {
            once: true,
          });
        });
      },
      maxAttempts: 2,
      requestTimeoutMs: 1,
      retryBackoffMs: 1,
    });

    await expect(generate(client)).rejects.toMatchObject({
      code: "generation_unavailable",
      retryable: true,
    });
    expect(attempts).toBe(2);
  });
});

function createClient(fetch: typeof globalThis.fetch, maxAttempts: number): OllamaClient {
  return new OllamaClient({
    baseUrl: "http://ollama.test",
    fetch,
    maxAttempts,
    requestTimeoutMs: 1_000,
    retryBackoffMs: 1,
  });
}

async function generate(
  client: OllamaClient,
  options: { readonly outputLimitBytes?: number; readonly signal?: AbortSignal } = {},
) {
  return client.generate({
    events: [],
    job,
    outputLimitBytes: options.outputLimitBytes ?? 64_000,
    signal: options.signal ?? new AbortController().signal,
  });
}

function ollamaResponse(content: string): Response {
  return Response.json({ message: { content } });
}

const job: SessionSummaryGenerationJob = {
  budgetClass: "standard",
  deadlineAt: "2099-07-17T00:00:00.000Z",
  expectedPrevious: { coversSeqTo: null, summaryId: null },
  inputLimitBytes: 64_000,
  kind: "session_summary.generate.v1",
  ollama: {
    contextSize: 32_768,
    model: "evaluated-model",
    quantization: "Q4_K_M",
    revision: "revision",
    thinkingMode: "disabled",
  },
  outputLimitBytes: 64_000,
  outputSchemaVersion: "session-summary.v1",
  previousSummary: null,
  producer: { id: "worker", version: "1" },
  promptVersion: "session-summary.v1",
  range: { from: 1, to: 1 },
  sessionId: "session-1",
  source: {
    eventCount: 1,
    firstEventId: "event-1",
    lastEventId: "event-1",
    rangeHash: "a".repeat(64),
  },
  summaryId: "summary-1",
  taskId: "task-1",
};
