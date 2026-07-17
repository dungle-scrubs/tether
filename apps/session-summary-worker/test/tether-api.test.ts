import type { SessionEvent } from "@dungle-scrubs/tether-protocol";
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
});

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
