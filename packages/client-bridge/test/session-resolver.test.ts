import { describe, expect, it } from "vitest";

import {
  ClientBridgeSessionResolver,
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
      currentSessionId: "sess_1",
      requestCount: 1,
      resolvedExternalIdCount: 1,
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
      requestCount: 1,
      resolvedExternalIdCount: 2,
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
    return new Response(JSON.stringify(response.body), {
      headers: { "content-type": "application/json" },
      status: response.status,
    });
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
