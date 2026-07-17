import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { URL } from "node:url";

import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type { AuthContext } from "../src/auth/token.js";
import { handleTaskHttpRoute } from "../src/http-task-route-handlers.js";
import type { SubscriptionHub } from "../src/hub.js";
import { computeScheduleWindow } from "../src/protocol.js";
import type { ResourceLimits } from "../src/resource-limits.js";
import type { SessionServiceEffect } from "../src/session-service.js";
import type {
  EnsureScheduledRunRequest,
  ScheduledRunEnsureResult,
  ScheduledSupersessionResult,
  SupersedeScheduledRunsRequest,
} from "../src/session-service-contracts.js";
import type { TaskRecord } from "../src/types.js";

function taskRecord(taskId: string): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-07-12T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: null,
    kind: "email_organization",
    objective: "Organize the mailbox",
    releasedAt: null,
    releasedBy: null,
    result: null,
    schedule: null,
    sessionId: "sess_mailbox_1",
    taskId,
  };
}

class RecordingHub {
  readonly broadcasts: unknown[] = [];
  broadcast(event: unknown): void {
    this.broadcasts.push(event);
  }
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

function jsonRequest(method: string, body: Record<string, unknown>): IncomingMessage {
  const request = Readable.from([Buffer.from(JSON.stringify(body))]) as unknown as IncomingMessage;
  (request as { method?: string }).method = method;
  return request;
}

function authContext(role: AuthContext["role"], participantId: string): AuthContext {
  return {
    expiresAt: "2099-01-01T00:00:00.000Z",
    grantJti: null,
    issuer: null,
    kid: "default",
    participantId,
    role,
    sessionScope: "*",
  };
}

const mailboxScope = { accountId: "acct_1", provider: "fastmail" };
const window = computeScheduleWindow(1_700_003_600_000, 3_600_000);

const supersedeBody = {
  candidateTaskIds: ["task_old_window"],
  kind: "email_organization",
  schedule: {
    mailboxAccountId: mailboxScope.accountId,
    mailboxProvider: mailboxScope.provider,
    scheduleAlgorithmVersion: window.algorithmVersion,
    scheduleIntervalMs: window.intervalMs,
    scheduleWindowStart: window.startMs,
  },
};

interface FakeServiceCalls {
  readonly ensureScheduledRun: EnsureScheduledRunRequest[];
  readonly supersedeScheduledRuns: SupersedeScheduledRunsRequest[];
}

function fakeService(input: {
  readonly calls: FakeServiceCalls;
  readonly supersede?: (
    request: SupersedeScheduledRunsRequest,
  ) => Effect.Effect<ScheduledSupersessionResult, unknown>;
  readonly ensure?: (
    request: EnsureScheduledRunRequest,
  ) => Effect.Effect<ScheduledRunEnsureResult, unknown>;
}): SessionServiceEffect {
  return {
    ensureScheduledRun: (request: EnsureScheduledRunRequest) => {
      input.calls.ensureScheduledRun.push(request);
      return (
        input.ensure?.(request) ?? Effect.die("ensureScheduledRun was not expected in this test")
      );
    },
    supersedeScheduledRuns: (request: SupersedeScheduledRunsRequest) => {
      input.calls.supersedeScheduledRuns.push(request);
      return (
        input.supersede?.(request) ??
        Effect.die("supersedeScheduledRuns was not expected in this test")
      );
    },
  } as unknown as SessionServiceEffect;
}

function runRoute(input: {
  readonly authContext: AuthContext | null;
  readonly body: Record<string, unknown>;
  readonly hub: SubscriptionHub;
  readonly method?: string;
  readonly path: string;
  readonly response: CapturingResponse;
  readonly service: SessionServiceEffect;
}): Promise<boolean> {
  return Effect.runPromise(
    handleTaskHttpRoute({
      authContext: input.authContext,
      hub: input.hub,
      request: jsonRequest(input.method ?? "POST", input.body),
      resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
      response: input.response as unknown as ServerResponse,
      service: input.service,
      url: new URL(`http://localhost${input.path}`),
    }),
  );
}

describe("scheduled-run supersession REST authorization", () => {
  const supersedePath = "/sessions/sess_mailbox_1/scheduled-runs/supersede";

  it("rejects a scheduler/participant task-mutate token with the typed role auth error", async () => {
    const calls: FakeServiceCalls = { ensureScheduledRun: [], supersedeScheduledRuns: [] };
    const response = new CapturingResponse();
    const service = fakeService({ calls });

    await runRoute({
      authContext: authContext("participant", "scheduler_1"),
      body: supersedeBody,
      hub: new RecordingHub() as unknown as SubscriptionHub,
      path: supersedePath,
      response,
      service,
    });

    // The operator gate must reject before any store work runs.
    expect(calls.supersedeScheduledRuns).toHaveLength(0);
    expect(response.statusCode).toBe(403);
    expect(response.body).toEqual({ error: "Forbidden", reason: "role" });
  });

  it("rejects an observer token from supersession", async () => {
    const calls: FakeServiceCalls = { ensureScheduledRun: [], supersedeScheduledRuns: [] };
    const response = new CapturingResponse();

    await runRoute({
      authContext: authContext("observer", "observer_1"),
      body: supersedeBody,
      hub: new RecordingHub() as unknown as SubscriptionHub,
      path: supersedePath,
      response,
      service: fakeService({ calls }),
    });

    expect(calls.supersedeScheduledRuns).toHaveLength(0);
    expect(response.statusCode).toBe(403);
    expect(response.body).toEqual({ error: "Forbidden", reason: "role" });
  });

  it("accepts an admin/operator token and records the authenticated operator as actor", async () => {
    const calls: FakeServiceCalls = { ensureScheduledRun: [], supersedeScheduledRuns: [] };
    const superseded = taskRecord("task_old_window");
    const hub = new RecordingHub();
    const response = new CapturingResponse();
    const service = fakeService({
      calls,
      supersede: () =>
        Effect.succeed({
          events: [],
          refusals: [],
          status: "applied",
          supersededTasks: [superseded],
        }),
    });

    await runRoute({
      authContext: authContext("admin", "operator_1"),
      body: supersedeBody,
      hub: hub as unknown as SubscriptionHub,
      path: supersedePath,
      response,
      service,
    });

    expect(calls.supersedeScheduledRuns).toHaveLength(1);
    const request = calls.supersedeScheduledRuns[0];
    if (!request) {
      throw new Error("expected a supersedeScheduledRuns request");
    }
    // The recorded actor comes from the authenticated operator identity, not the body.
    expect(request.participantId).toBe("operator_1");
    expect(request.identity.kind).toBe("email_organization");
    expect(request.identity.mailboxScope).toEqual(mailboxScope);
    expect(request.identity.scheduleWindow.startMs).toBe(window.startMs);
    expect(request.candidateTaskIds).toEqual(["task_old_window"]);
    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      refusals: [],
      status: "applied",
      supersededTasks: [superseded],
    });
  });

  it("still authorizes a scheduled create for the scheduler/participant role", async () => {
    const calls: FakeServiceCalls = { ensureScheduledRun: [], supersedeScheduledRuns: [] };
    const current = taskRecord("task_current_window");
    const hub = new RecordingHub();
    const response = new CapturingResponse();
    const service = fakeService({
      calls,
      ensure: () =>
        Effect.succeed({
          created: true,
          current,
          events: [],
          status: "ensured",
          supersededTasks: [],
          taskId: current.taskId,
        }),
    });

    await runRoute({
      authContext: authContext("participant", "scheduler_1"),
      body: {
        kind: "email_organization",
        objective: "Organize the mailbox",
        schedule: {
          mailboxAccountId: mailboxScope.accountId,
          mailboxProvider: mailboxScope.provider,
          scheduleAlgorithmVersion: window.algorithmVersion,
          scheduleIntervalMs: window.intervalMs,
          scheduleWindowStart: window.startMs,
        },
      },
      hub: hub as unknown as SubscriptionHub,
      path: "/sessions/sess_mailbox_1/tasks",
      response,
      service,
    });

    // The deterministic scheduled create stays a task-mutate operation for the scheduler.
    expect(calls.supersedeScheduledRuns).toHaveLength(0);
    expect(calls.ensureScheduledRun).toHaveLength(1);
    expect(response.statusCode).toBe(201);
    expect(response.body).toEqual({ status: "created", task: current });
  });
});
