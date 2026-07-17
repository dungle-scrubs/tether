import type { IncomingMessage, ServerResponse } from "node:http";
import { URL } from "node:url";

import { Effect } from "effect";
import { describe, expect, it, vi } from "vitest";

import type { AuthGrantLifecycle } from "../src/auth/grant-lifecycle.js";
import { type AuthContext, AuthError } from "../src/auth/token.js";
import { handleAuthGrantHttpRoute } from "../src/http-auth-grant-route-handlers.js";
import { handleSessionDebugHttpRoute } from "../src/http-session-debug-route-handlers.js";
import type { SessionServiceEffect } from "../src/session-service.js";

describe("admin session scope on HTTP surfaces", () => {
  it("denies a session-scoped admin on another session's debug routes", async () => {
    const service = createDebugSessionService();
    const response = createResponseRecorder();

    const handled = await Effect.runPromise(
      handleSessionDebugHttpRoute({
        authContext: adminContext("sess_A"),
        request: { method: "GET" } as IncomingMessage,
        response: response.response,
        service: service.service,
        url: new URL("http://localhost/sessions/sess_B/debug/participants"),
      }),
    );

    expect(handled).toBe(true);
    expect(response.statusCode()).toBe(403);
    expect(response.body()).toMatchObject({ reason: AuthError.ScopeDenied });
    expect(service.listParticipantRuntimeSnapshots).not.toHaveBeenCalled();
  });

  it("accepts a session-scoped admin on its own session's debug routes", async () => {
    const service = createDebugSessionService();
    const response = createResponseRecorder();

    const handled = await Effect.runPromise(
      handleSessionDebugHttpRoute({
        authContext: adminContext("sess_A"),
        request: { method: "GET" } as IncomingMessage,
        response: response.response,
        service: service.service,
        url: new URL("http://localhost/sessions/sess_A/debug/participants"),
      }),
    );

    expect(handled).toBe(true);
    expect(response.statusCode()).toBe(200);
    expect(service.listParticipantRuntimeSnapshots).toHaveBeenCalledWith("sess_A");
  });

  it("accepts a service-scoped admin on any session's debug routes", async () => {
    const service = createDebugSessionService();
    const response = createResponseRecorder();

    const handled = await Effect.runPromise(
      handleSessionDebugHttpRoute({
        authContext: adminContext("*"),
        request: { method: "GET" } as IncomingMessage,
        response: response.response,
        service: service.service,
        url: new URL("http://localhost/sessions/sess_B/debug/tasks"),
      }),
    );

    expect(handled).toBe(true);
    expect(response.statusCode()).toBe(200);
    expect(service.listTaskSnapshots).toHaveBeenCalledWith("sess_B");
  });

  it("denies a session-scoped admin on the global grant lifecycle surface", async () => {
    const lifecycle = createGrantLifecycle();
    const response = createResponseRecorder();

    const handled = await Effect.runPromise(
      handleAuthGrantHttpRoute({
        authContext: adminContext("sess_A"),
        lifecycle: lifecycle.lifecycle,
        maxBodyBytes: 65_536,
        request: { method: "GET" } as IncomingMessage,
        response: response.response,
        url: new URL("http://localhost/auth/grants"),
      }),
    );

    expect(handled).toBe(true);
    expect(response.statusCode()).toBe(403);
    expect(response.body()).toMatchObject({ reason: AuthError.ScopeDenied });
    expect(lifecycle.list).not.toHaveBeenCalled();
  });

  it("denies a session-scoped admin revoking grants", async () => {
    const lifecycle = createGrantLifecycle();
    const response = createResponseRecorder();

    const handled = await Effect.runPromise(
      handleAuthGrantHttpRoute({
        authContext: adminContext("sess_A"),
        lifecycle: lifecycle.lifecycle,
        maxBodyBytes: 65_536,
        request: { method: "POST" } as IncomingMessage,
        response: response.response,
        url: new URL("http://localhost/auth/grants/grant_target/revoke"),
      }),
    );

    expect(handled).toBe(true);
    expect(response.statusCode()).toBe(403);
    expect(response.body()).toMatchObject({ reason: AuthError.ScopeDenied });
    expect(lifecycle.revoke).not.toHaveBeenCalled();
  });

  it("accepts a service-scoped admin on the global grant lifecycle surface", async () => {
    const lifecycle = createGrantLifecycle();
    const response = createResponseRecorder();

    const handled = await Effect.runPromise(
      handleAuthGrantHttpRoute({
        authContext: adminContext("*"),
        lifecycle: lifecycle.lifecycle,
        maxBodyBytes: 65_536,
        request: { method: "GET" } as IncomingMessage,
        response: response.response,
        url: new URL("http://localhost/auth/grants"),
      }),
    );

    expect(handled).toBe(true);
    expect(response.statusCode()).toBe(200);
    expect(lifecycle.list).toHaveBeenCalledWith(50);
  });
});

function adminContext(sessionScope: string): AuthContext {
  return {
    expiresAt: "2099-01-01T00:00:00.000Z",
    grantJti: "grant_admin_scope_test",
    issuer: "https://auth.scope.test",
    kid: "default",
    participantId: "part_admin_scope",
    role: "admin",
    sessionScope,
  };
}

function createResponseRecorder(): {
  readonly response: ServerResponse;
  readonly statusCode: () => number | null;
  readonly body: () => unknown;
} {
  let statusCode: number | null = null;
  let rawBody: string | null = null;
  const response = {
    end: (chunk?: unknown) => {
      if (typeof chunk === "string") {
        rawBody = chunk;
      }
    },
    writeHead: (status: number) => {
      statusCode = status;
    },
  } as unknown as ServerResponse;
  return {
    body: () => (rawBody === null ? null : (JSON.parse(rawBody) as unknown)),
    response,
    statusCode: () => statusCode,
  };
}

function createDebugSessionService() {
  const listParticipantRuntimeSnapshots = vi.fn((_sessionId: string) => Effect.succeed([]));
  const listTaskSnapshots = vi.fn((_sessionId: string) => Effect.succeed([]));
  const service = {
    listParticipantRuntimeSnapshots,
    listTaskSnapshots,
  } as unknown as SessionServiceEffect;
  return { listParticipantRuntimeSnapshots, listTaskSnapshots, service };
}

function createGrantLifecycle() {
  const list = vi.fn(async () => []);
  const revoke = vi.fn(async () => ({ grant: null }));
  const lifecycle = {
    issuanceEnabled: true,
    list,
    revoke,
  } as unknown as AuthGrantLifecycle;
  return { lifecycle, list, revoke };
}
