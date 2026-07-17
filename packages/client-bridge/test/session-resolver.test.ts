import { describe, expect, it } from "vitest";

import {
  ClientBridgeSessionResolver,
  ClientBridgeSessionResolverResourceLimitError,
  type ClientBridgeRequestError,
  type ClientBridgeFetch,
} from "../src/index.js";

describe("ClientBridgeSessionResolver", () => {
  it("resolves a provider conversation and caches the session id", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          binding: createBindingFixture({ externalId: "chat_1", sessionId: "sess_1" }),
          created: true,
          session: createSessionFixture({ sessionId: "sess_1" }),
        },
        status: 200,
      },
    ]);
    const resolver = new ClientBridgeSessionResolver(
      {
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch },
    );

    await expect(resolver.ensureSessionId("chat_1")).resolves.toBe("sess_1");
    await expect(resolver.ensureSessionId("chat_1")).resolves.toBe("sess_1");

    expect(requests).toEqual([
      {
        body: {
          externalId: "chat_1",
          provider: "external-chat",
        },
        method: "POST",
        path: "/client-bindings/session",
      },
    ]);
    expect(resolver.debugInfo()).toEqual({
      cacheCapacity: 1_024,
      cacheSize: 1,
      currentSessionId: "sess_1",
      evictionCount: 0,
      expiryCount: 0,
      inFlightCount: 0,
      lastOutcome: "cache-hit",
      maxInFlight: 64,
      requestCount: 1,
    });
  });

  it("passes a default session id when a bridge is configured with one", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          binding: createBindingFixture({
            externalId: "C123",
            provider: "slack",
            sessionId: "sess_existing",
          }),
          created: false,
          session: createSessionFixture({ sessionId: "sess_existing" }),
        },
        status: 200,
      },
    ]);
    const resolver = new ClientBridgeSessionResolver(
      {
        defaultSessionId: "sess_existing",
        provider: "slack",
        serviceUrl: "http://tether.test",
      },
      { fetch },
    );

    await expect(resolver.resolveSession("C123")).resolves.toMatchObject({
      created: false,
      session: { sessionId: "sess_existing" },
    });

    expect(requests[0]?.body).toEqual({
      externalId: "C123",
      provider: "slack",
      sessionId: "sess_existing",
    });
  });

  it("lists provider bindings and seeds the local cache", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          bindings: [
            createBindingFixture({ externalId: "chat_1", sessionId: "sess_1" }),
            createBindingFixture({ externalId: "chat_2", sessionId: "sess_2" }),
          ],
        },
        status: 200,
      },
    ]);
    const resolver = new ClientBridgeSessionResolver(
      {
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch },
    );

    const bindings = await resolver.listBindings();

    expect(bindings.map((binding) => binding.sessionId)).toEqual(["sess_1", "sess_2"]);
    expect(requests).toEqual([
      {
        body: null,
        method: "GET",
        path: "/client-bindings?provider=external-chat",
      },
    ]);
    await expect(resolver.ensureSessionId("chat_2")).resolves.toBe("sess_2");
    expect(resolver.debugInfo()).toMatchObject({
      cacheSize: 2,
      requestCount: 1,
    });
  });

  it("returns typed errors for invalid Tether responses", async () => {
    const resolver = new ClientBridgeSessionResolver(
      {
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      {
        fetch: async () =>
          new Response(JSON.stringify({ nope: true }), {
            headers: { "content-type": "application/json" },
            status: 200,
          }),
      },
    );

    await expect(resolver.ensureSessionId("chat_1")).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      name: "ClientBridgeRequestError",
    } satisfies Partial<ClientBridgeRequestError>);
  });

  it("reports a fully bounded debug shape before any activity", () => {
    const resolver = new ClientBridgeSessionResolver({
      provider: "external-chat",
      serviceUrl: "http://tether.test",
    });

    expect(resolver.debugInfo()).toEqual({
      cacheCapacity: 1_024,
      cacheSize: 0,
      currentSessionId: null,
      evictionCount: 0,
      expiryCount: 0,
      inFlightCount: 0,
      lastOutcome: null,
      maxInFlight: 64,
      requestCount: 0,
    });
  });

  it("evicts the least-recently-used entry once capacity is exceeded", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      resolveResponse("chat_a", "sess_a"),
      resolveResponse("chat_b", "sess_b"),
      resolveResponse("chat_c", "sess_c"),
      resolveResponse("chat_b", "sess_b2"),
    ]);
    const resolver = new ClientBridgeSessionResolver(
      {
        cacheCapacity: 2,
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch },
    );

    await resolver.ensureSessionId("chat_a");
    await resolver.ensureSessionId("chat_b");
    // Read chat_a so it becomes most-recently-used and outlives the next write.
    await expect(resolver.ensureSessionId("chat_a")).resolves.toBe("sess_a");
    await resolver.ensureSessionId("chat_c");

    expect(resolver.debugInfo()).toMatchObject({
      cacheSize: 2,
      evictionCount: 1,
      lastOutcome: "evicted",
    });
    // chat_a was refreshed by the read, so it is still a cache hit (no fetch).
    await expect(resolver.ensureSessionId("chat_a")).resolves.toBe("sess_a");
    expect(requests).toHaveLength(3);
    // chat_b was the LRU key and was evicted, so it must be re-resolved.
    await expect(resolver.ensureSessionId("chat_b")).resolves.toBe("sess_b2");
    expect(requests).toHaveLength(4);
  });

  it("expires an entry after it sits idle beyond the ttl", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      resolveResponse("chat_a", "sess_a"),
      resolveResponse("chat_a", "sess_a2"),
    ]);
    const clock = createClock(0);
    const resolver = new ClientBridgeSessionResolver(
      {
        idleTtlMs: 1_000,
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { clock, fetch },
    );

    await resolver.ensureSessionId("chat_a");
    clock.advance(500);
    // Within the ttl: served from cache, recency refreshed to now.
    await expect(resolver.ensureSessionId("chat_a")).resolves.toBe("sess_a");
    expect(requests).toHaveLength(1);

    clock.advance(1_500);
    // Idle beyond ttl relative to the refreshed access: expired and re-resolved.
    await expect(resolver.ensureSessionId("chat_a")).resolves.toBe("sess_a2");
    expect(requests).toHaveLength(2);
    expect(resolver.debugInfo()).toMatchObject({ cacheSize: 1, expiryCount: 1 });
  });

  it("invalidates exactly one mapping", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      resolveResponse("chat_a", "sess_a"),
      resolveResponse("chat_b", "sess_b"),
      resolveResponse("chat_a", "sess_a2"),
    ]);
    const resolver = new ClientBridgeSessionResolver(
      {
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch },
    );

    await resolver.ensureSessionId("chat_a");
    await resolver.ensureSessionId("chat_b");

    resolver.invalidate("chat_a");
    expect(resolver.debugInfo()).toMatchObject({ cacheSize: 1, lastOutcome: "invalidated" });

    // chat_b survives the invalidation and is still a cache hit.
    await expect(resolver.ensureSessionId("chat_b")).resolves.toBe("sess_b");
    expect(requests).toHaveLength(2);
    // chat_a was removed and must be re-resolved.
    await expect(resolver.ensureSessionId("chat_a")).resolves.toBe("sess_a2");
    expect(requests).toHaveLength(3);
  });

  it("joins same-key concurrent resolutions into a single fetch", async () => {
    const deferred = createDeferredFetch();
    const resolver = new ClientBridgeSessionResolver(
      {
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch: deferred.fetch },
    );

    const first = resolver.resolveSession("chat_1");
    const second = resolver.resolveSession("chat_1");

    expect(deferred.callCount()).toBe(1);
    expect(resolver.debugInfo().lastOutcome).toBe("in-flight-joined");
    expect(resolver.debugInfo().inFlightCount).toBe(1);

    deferred.resolveNext(jsonResponse(resolveResponse("chat_1", "sess_1").body));
    await expect(first).resolves.toMatchObject({ session: { sessionId: "sess_1" } });
    await expect(second).resolves.toMatchObject({ session: { sessionId: "sess_1" } });
    expect(deferred.callCount()).toBe(1);
    expect(resolver.debugInfo()).toMatchObject({ cacheSize: 1, inFlightCount: 0 });
  });

  it("joins a same-key ensureSessionId miss onto an in-flight resolution", async () => {
    const deferred = createDeferredFetch();
    const resolver = new ClientBridgeSessionResolver(
      {
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch: deferred.fetch },
    );

    const first = resolver.ensureSessionId("chat_1");
    const second = resolver.ensureSessionId("chat_1");

    expect(deferred.callCount()).toBe(1);
    expect(resolver.debugInfo().lastOutcome).toBe("in-flight-joined");

    deferred.resolveNext(jsonResponse(resolveResponse("chat_1", "sess_1").body));
    await expect(first).resolves.toBe("sess_1");
    await expect(second).resolves.toBe("sess_1");
    expect(deferred.callCount()).toBe(1);
  });

  it("rejects a new distinct resolution beyond the in-flight cap without fetching", async () => {
    const deferred = createDeferredFetch();
    const resolver = new ClientBridgeSessionResolver(
      {
        maxInFlight: 2,
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch: deferred.fetch },
    );

    const first = resolver.resolveSession("chat_1");
    const second = resolver.resolveSession("chat_2");
    expect(deferred.callCount()).toBe(2);

    await expect(resolver.resolveSession("chat_3")).rejects.toBeInstanceOf(
      ClientBridgeSessionResolverResourceLimitError,
    );
    await expect(resolver.resolveSession("chat_3")).rejects.toMatchObject({ maxInFlight: 2 });
    // The overflowing key never reached the transport.
    expect(deferred.callCount()).toBe(2);
    expect(resolver.debugInfo()).toMatchObject({
      inFlightCount: 2,
      lastOutcome: "in-flight-overflow",
    });

    deferred.resolveNext(jsonResponse(resolveResponse("chat_1", "sess_1").body));
    deferred.resolveNext(jsonResponse(resolveResponse("chat_2", "sess_2").body));
    await Promise.all([first, second]);
  });

  it("clears the in-flight entry when a resolution fails so a retry proceeds", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      { body: { code: "boom" }, status: 500 },
      resolveResponse("chat_1", "sess_1"),
    ]);
    const resolver = new ClientBridgeSessionResolver(
      {
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch },
    );

    await expect(resolver.resolveSession("chat_1")).rejects.toMatchObject({
      code: "HTTP_ERROR",
    });
    expect(resolver.debugInfo().inFlightCount).toBe(0);

    // A stuck in-flight entry would have blocked or joined this call; instead it retries.
    await expect(resolver.resolveSession("chat_1")).resolves.toMatchObject({
      session: { sessionId: "sess_1" },
    });
    expect(requests).toHaveLength(2);
  });

  it("routes listBindings seeding through the bounded LRU admission path", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          bindings: [
            createBindingFixture({ externalId: "chat_1", sessionId: "sess_1" }),
            createBindingFixture({ externalId: "chat_2", sessionId: "sess_2" }),
            createBindingFixture({ externalId: "chat_3", sessionId: "sess_3" }),
          ],
        },
        status: 200,
      },
      resolveResponse("chat_1", "sess_1b"),
    ]);
    const resolver = new ClientBridgeSessionResolver(
      {
        cacheCapacity: 2,
        provider: "external-chat",
        serviceUrl: "http://tether.test",
      },
      { fetch },
    );

    await resolver.listBindings();

    expect(resolver.debugInfo()).toMatchObject({
      cacheSize: 2,
      evictionCount: 1,
      lastOutcome: "evicted",
    });
    // chat_1 was the LRU seed and got evicted; it must be re-resolved.
    await expect(resolver.ensureSessionId("chat_1")).resolves.toBe("sess_1b");
    // chat_3 was retained by the bound and remains a cache hit.
    await expect(resolver.ensureSessionId("chat_3")).resolves.toBe("sess_3");
    expect(requests).toHaveLength(2);
  });

  it("rejects non-positive, non-finite, or non-integer bounds", () => {
    const base = {
      provider: "external-chat",
      serviceUrl: "http://tether.test",
    } as const;

    for (const cacheCapacity of [0, -1, 1.5, Number.POSITIVE_INFINITY, Number.NaN]) {
      expect(() => new ClientBridgeSessionResolver({ ...base, cacheCapacity })).toThrow(RangeError);
    }
    for (const idleTtlMs of [0, -5, Number.POSITIVE_INFINITY, Number.NaN]) {
      expect(() => new ClientBridgeSessionResolver({ ...base, idleTtlMs })).toThrow(RangeError);
    }
    for (const maxInFlight of [0, -3, 2.5, Number.POSITIVE_INFINITY, Number.NaN]) {
      expect(() => new ClientBridgeSessionResolver({ ...base, maxInFlight })).toThrow(RangeError);
    }
  });
});

interface CapturedRequest {
  readonly body: unknown;
  readonly method: string;
  readonly path: string;
}

interface JsonResponse {
  readonly body: unknown;
  readonly status: number;
}

/** Controllable clock injected for deterministic idle-expiry tests. */
interface TestClock {
  /** Advances the clock by the supplied number of milliseconds. */
  readonly advance: (deltaMs: number) => void;
  /** Returns the current epoch-millisecond time. */
  readonly now: () => number;
}

/** Deferred fetch stub that resolves queued responses on demand. */
interface DeferredFetch {
  /** Returns how many times the fetch stub was invoked. */
  readonly callCount: () => number;
  /** Fetch implementation whose responses are released manually. */
  readonly fetch: ClientBridgeFetch;
  /** Resolves the oldest pending fetch with the supplied response. */
  readonly resolveNext: (response: Response) => void;
}

/**
 * Creates a fetch implementation that records requests and returns queued JSON
 * responses.
 */
function createJsonFetch(
  requests: CapturedRequest[],
  responses: readonly JsonResponse[],
): ClientBridgeFetch {
  let index = 0;
  return async (url, init) => {
    requests.push({
      body: typeof init.body === "string" ? JSON.parse(init.body) : null,
      method: init.method ?? "GET",
      path: `${url.pathname}${url.search}`,
    });
    const response = responses[index];
    index += 1;
    if (!response) {
      throw new Error("No queued response");
    }
    return jsonResponse(response.body, response.status);
  };
}

/** Creates a controllable clock starting at the supplied epoch millisecond. */
function createClock(startMs: number): TestClock {
  let current = startMs;
  return {
    advance: (deltaMs) => {
      current += deltaMs;
    },
    now: () => current,
  };
}

/** Creates a fetch whose responses are released one at a time. */
function createDeferredFetch(): DeferredFetch {
  const resolvers: Array<(response: Response) => void> = [];
  let callCount = 0;
  return {
    callCount: () => callCount,
    fetch: () => {
      callCount += 1;
      return new Promise<Response>((resolve) => {
        resolvers.push(resolve);
      });
    },
    resolveNext: (response) => {
      const resolve = resolvers.shift();
      if (!resolve) {
        throw new Error("No pending fetch to resolve");
      }
      resolve(response);
    },
  };
}

/** Builds a JSON `Response` with the shared content type used by Tether. */
function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status,
  });
}

/** Builds a queued session-resolution response for one external id. */
function resolveResponse(externalId: string, sessionId: string): JsonResponse {
  return {
    body: {
      binding: createBindingFixture({ externalId, sessionId }),
      created: true,
      session: createSessionFixture({ sessionId }),
    },
    status: 200,
  };
}

/**
 * Builds a binding fixture returned by Tether.
 */
function createBindingFixture(input: {
  readonly externalId: string;
  readonly provider?: string;
  readonly sessionId: string;
}): {
  readonly archivedAt: string | null;
  readonly createdAt: string;
  readonly externalId: string;
  readonly lastSeenAt: string;
  readonly provider: string;
  readonly sessionId: string;
} {
  return {
    archivedAt: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    externalId: input.externalId,
    lastSeenAt: "2026-01-01T00:00:00.000Z",
    provider: input.provider ?? "external-chat",
    sessionId: input.sessionId,
  };
}

/**
 * Builds a session fixture returned by Tether.
 */
function createSessionFixture(input: { readonly sessionId: string }): {
  readonly archivedAt: string | null;
  readonly createdAt: string;
  readonly sessionId: string;
} {
  return {
    archivedAt: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    sessionId: input.sessionId,
  };
}
