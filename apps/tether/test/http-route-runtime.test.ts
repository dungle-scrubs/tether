import type { ServerResponse } from "node:http";

import { describe, expect, it } from "vitest";
import { z } from "zod";

import { AuthError } from "../src/auth/token.js";
import {
  controlLeaseConflictError,
  handleHttpRouteError,
  sendAuthError,
  type HttpRouteErrorLogDetails,
  type HttpRouteErrorLogger,
} from "../src/http-route-runtime.js";

describe("handleHttpRouteError", () => {
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
