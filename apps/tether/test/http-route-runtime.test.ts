import type { ServerResponse } from "node:http";

import { describe, expect, it } from "vitest";
import { z } from "zod";

import { AuthError } from "../src/auth/token.js";
import { projectHealthResponse } from "../src/http.js";
import { fencedHttpRouteNames, httpRouteInventory } from "../src/http-route-inventory.js";
import {
  controlLeaseConflictError,
  type HttpRouteErrorLogDetails,
  type HttpRouteErrorLogger,
  handleHttpRouteError,
  readCorsOptionsFromEnv,
  sendAuthError,
  sendControlEpochRequired,
} from "../src/http-route-runtime.js";
import { RestControlPolicy } from "../src/rest-control-policy.js";

describe("handleHttpRouteError", () => {
  it("accepts only canonical HTTP origins in browser authority configuration", () => {
    expect(
      readCorsOptionsFromEnv({
        BROWSER_ALLOWED_ORIGINS: "https://Hub.Example.Test, http://127.0.0.1:17445/",
      }),
    ).toEqual({
      allowedOrigins: ["https://hub.example.test", "http://127.0.0.1:17445"],
    });

    for (const value of [
      "null",
      "*",
      "file:///tmp/hub",
      "https://user@example.test",
      "https://example.test/path",
      "https://example.test?query=1",
      "https://example.test#fragment",
      "not-an-origin",
    ]) {
      expect(() => readCorsOptionsFromEnv({ BROWSER_ALLOWED_ORIGINS: value })).toThrowError(
        "browser_allowed_origin_invalid",
      );
    }
  });

  it("classifies every declared route and inventories participant-owned mutations", () => {
    expect(httpRouteInventory.every((route) => route.control.length > 0)).toBe(true);
    expect(
      httpRouteInventory
        .filter((route) => route.control === "acquisition")
        .map((route) => route.name),
    ).toEqual(["session.participant.register"]);
    expect(fencedHttpRouteNames).toEqual([
      "session.events.append",
      "session.participant.control.release",
      "session.participant.heartbeat",
      "session.summary.candidate.submit",
      "task.approval",
      "task.cancel",
      "task.claim",
      "task.claim.refresh",
      "task.complete",
      "task.fail",
      "task.release",
    ]);
  });

  it("returns a distinct required response for an enforced missing epoch", () => {
    const response = createJsonResponseRecorder();
    const policy = new RestControlPolicy(true);

    expect(
      policy.authorize({
        controlEpoch: undefined,
        routeName: "task.cancel",
      }),
    ).toEqual({ status: "control_epoch_required" });
    policy.record("task.cancel", "epoch_required");
    sendControlEpochRequired(response.response);

    expect(response.statusCode).toBe(428);
    expect(response.body).toEqual({
      code: "CONTROL_EPOCH_REQUIRED",
      error: "Control epoch is required",
      recovery: "acquire_control",
    });
    expect(policy.debugInfo()).toMatchObject({
      counts: { "task.cancel": { epoch_required: 1 } },
      mode: "enforced",
    });
  });

  it("accepts a compatibility missing epoch without inventing durable context", () => {
    const policy = new RestControlPolicy(false);

    expect(
      policy.authorize({
        controlEpoch: undefined,
        routeName: "task.cancel",
      }),
    ).toEqual({ status: "accepted_unfenced" });
    policy.record("task.cancel", "unfenced_accepted");
    expect(policy.debugInfo()).toMatchObject({
      counts: { "task.cancel": { unfenced_accepted: 1 } },
      lastFailure: null,
      mode: "compatibility",
    });
  });

  it("keeps compatibility health ready with one stable warning", () => {
    const compatibility = new RestControlPolicy(false);
    const enforced = new RestControlPolicy(true);

    expect(projectHealthResponse(compatibility.debugInfo())).toEqual({
      ok: true,
      warnings: [
        "summary_publication_disabled",
        "summary_worker_disabled",
        "ollama_disabled",
        "retention_disabled",
        "REST_CONTROL_COMPATIBILITY_ENABLED",
      ],
    });
    expect(projectHealthResponse(enforced.debugInfo())).toEqual({
      ok: true,
      warnings: [
        "summary_publication_disabled",
        "summary_worker_disabled",
        "ollama_disabled",
        "retention_disabled",
      ],
    });
  });

  it("projects live scalability warnings without changing health readiness", () => {
    const enforced = new RestControlPolicy(true);

    expect(
      projectHealthResponse(enforced.debugInfo(), [
        "projection_stale",
        "summary_invalid",
        "ollama_disabled",
      ]),
    ).toEqual({
      ok: true,
      warnings: ["projection_stale", "summary_invalid", "ollama_disabled"],
    });
  });

  it("redacts unexpected Error details from public 500 responses and logs them with a request id", () => {
    const response = createJsonResponseRecorder();
    const logs: RouteErrorLog[] = [];
    const secret = "secret db detail: unique constraint sessions_session_id_key";

    handleHttpRouteError(response.response, new Error(secret), {
      logger: collectRouteErrors(logs),
      requestIdFactory: () => "req_test_1",
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: "Internal server error",
      requestId: "req_test_1",
    });
    expect(JSON.stringify(response.body)).not.toContain(secret);
    expect(logs).toEqual([
      {
        details: {
          error: {
            message: secret,
            name: "Error",
          },
          requestId: "req_test_1",
        },
        event: "http.route_error",
      },
    ]);
  });

  it("redacts unknown thrown values from public 500 responses and server logs", () => {
    const response = createJsonResponseRecorder();
    const logs: RouteErrorLog[] = [];
    const secret = "raw thrown secret";

    handleHttpRouteError(
      response.response,
      { secret },
      {
        logger: collectRouteErrors(logs),
        requestIdFactory: () => "req_test_unknown",
      },
    );

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: "Internal server error",
      requestId: "req_test_unknown",
    });
    expect(JSON.stringify(response.body)).not.toContain(secret);
    expect(JSON.stringify(logs)).not.toContain(secret);
    expect(logs).toEqual([
      {
        details: {
          error: {
            message: "Non-Error thrown value",
            name: "UnknownError",
          },
          requestId: "req_test_unknown",
        },
        event: "http.route_error",
      },
    ]);
  });

  it("preserves Zod validation responses without generic 500 request ids", () => {
    const response = createJsonResponseRecorder();
    const parseResult = z.object({ sessionId: z.string() }).safeParse({});

    expect(parseResult.success).toBe(false);

    if (!parseResult.success) {
      handleHttpRouteError(response.response, parseResult.error, {
        requestIdFactory: () => "req_should_not_be_used",
      });
    }

    expect(response.statusCode).toBe(400);
    expect(response.body).toMatchObject({
      error: "Invalid request",
      issues: expect.any(Array),
    });
    expect(response.body).not.toHaveProperty("requestId");
  });

  it("preserves auth error responses without generic 500 request ids", () => {
    const response = createJsonResponseRecorder();

    sendAuthError(response.response, AuthError.Missing);

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: "Unauthorized",
      reason: "missing",
    });
    expect(response.body).not.toHaveProperty("requestId");
  });

  it("preserves explicit participant control conflict responses", () => {
    const conflict = controlLeaseConflictError(
      {
        activeLease: {
          claimedAt: "2026-07-06T00:00:00.000Z",
          controlChannel: "ws",
          epoch: 1,
          instanceId: "inst_active",
          lastSeenAt: "2026-07-06T00:00:30.000Z",
          leaseExpiresAt: "2026-07-06T00:01:00.000Z",
          participantId: "part_active",
          releasedAt: null,
          sessionId: "sess_conflict",
        },
        status: "conflict",
      },
      "rest",
    );

    expect(conflict).toEqual({
      activeControlChannel: "ws",
      code: "CONTROL_CONFLICT",
      error: "Participant already has an active control channel",
      instanceId: "inst_active",
      leaseExpiresAt: "2026-07-06T00:01:00.000Z",
      participantId: "part_active",
      requestedControlChannel: "rest",
    });
    expect(conflict).not.toHaveProperty("requestId");
  });
});

interface JsonResponseRecorder {
  readonly body: Record<string, unknown>;
  readonly response: ServerResponse;
  readonly statusCode: number | null;
}

interface RouteErrorLog {
  readonly details: HttpRouteErrorLogDetails;
  readonly event: "http.route_error";
}

function collectRouteErrors(logs: RouteErrorLog[]): HttpRouteErrorLogger {
  return {
    error: (event, details) => {
      logs.push({ details, event });
    },
  };
}

function createJsonResponseRecorder(): JsonResponseRecorder {
  const state: {
    body: Record<string, unknown>;
    statusCode: number | null;
  } = {
    body: {},
    statusCode: null,
  };
  const response = {
    end: (body: string) => {
      const parsed = JSON.parse(body) as unknown;
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error("Expected JSON object response body");
      }
      state.body = parsed as Record<string, unknown>;
    },
    writeHead: (statusCode: number) => {
      state.statusCode = statusCode;
    },
  } as unknown as ServerResponse;
  return {
    get body() {
      return state.body;
    },
    response,
    get statusCode() {
      return state.statusCode;
    },
  };
}
