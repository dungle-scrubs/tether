import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { URL } from "node:url";

import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import { ScheduledTaskIdentityMismatchError } from "../src/db.js";
import { handleTaskHttpRoute } from "../src/http-task-route-handlers.js";
import type { SubscriptionHub } from "../src/hub.js";
import { computeScheduleWindow, deriveScheduledTaskId } from "../src/protocol.js";
import type { ResourceLimits } from "../src/resource-limits.js";
import type { SessionServiceEffect } from "../src/session-service.js";
import {
  type EnsureScheduledRunRequest,
  type ScheduledRunEnsureResult,
  SessionServicePersistenceError,
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

const mailboxScope = { accountId: "acct_1", provider: "fastmail" };
const window = computeScheduleWindow(1_700_003_600_000, 3_600_000);
const derivedTaskId = deriveScheduledTaskId({
  kind: "email_organization",
  mailboxScope,
  scheduleWindow: window,
  sessionId: "sess_mailbox_1",
});

interface FakeServiceCalls {
  readonly createTask: unknown[];
  readonly ensureScheduledRun: EnsureScheduledRunRequest[];
}

function fakeService(input: {
  readonly calls: FakeServiceCalls;
  readonly ensure: (
    request: EnsureScheduledRunRequest,
  ) => Effect.Effect<ScheduledRunEnsureResult, unknown>;
}): SessionServiceEffect {
  return {
    createTask: (request: unknown) => {
      (input.calls.createTask as unknown[]).push(request);
      return Effect.die("createTask must not be used for scheduled creates");
    },
    ensureScheduledRun: (request: EnsureScheduledRunRequest) => {
      (input.calls.ensureScheduledRun as EnsureScheduledRunRequest[]).push(request);
      return input.ensure(request);
    },
  } as unknown as SessionServiceEffect;
}

function runCreate(input: {
  readonly body: Record<string, unknown>;
  readonly service: SessionServiceEffect;
  readonly hub: SubscriptionHub;
  readonly response: CapturingResponse;
}): Promise<boolean> {
  return Effect.runPromise(
    handleTaskHttpRoute({
      authContext: null,
      hub: input.hub,
      request: jsonRequest("POST", input.body),
      resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
      response: input.response as unknown as ServerResponse,
      service: input.service,
      url: new URL("http://localhost/sessions/sess_mailbox_1/tasks"),
    }),
  );
}

describe("scheduled task REST create routing", () => {
  it("rejects a scheduled create whose derived window end is unsafe", async () => {
    const calls: FakeServiceCalls = { createTask: [], ensureScheduledRun: [] };
    const response = new CapturingResponse();
    const service = fakeService({
      calls,
      ensure: () => Effect.die("unsafe scheduled creates must fail before service dispatch"),
    });

    await runCreate({
      body: {
        kind: "email_organization",
        objective: "Organize the mailbox",
        schedule: {
          mailboxAccountId: mailboxScope.accountId,
          mailboxProvider: mailboxScope.provider,
          scheduleAlgorithmVersion: 1,
          scheduleIntervalMs: 2,
          scheduleWindowStart: Number.MAX_SAFE_INTEGER - 1,
        },
      },
      hub: new RecordingHub() as unknown as SubscriptionHub,
      response,
      service,
    });

    expect(calls.createTask).toHaveLength(0);
    expect(calls.ensureScheduledRun).toHaveLength(0);
    expect(response.statusCode).toBe(400);
  });

  it("routes a scheduled create through the atomic ensure-scheduled-run path", async () => {
    const calls: FakeServiceCalls = { createTask: [], ensureScheduledRun: [] };
    const current = taskRecord(derivedTaskId);
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
          taskId: derivedTaskId,
        }),
    });

    await runCreate({
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
        taskId: derivedTaskId,
      },
      hub: hub as unknown as SubscriptionHub,
      response,
      service,
    });

    // The scheduled create must never fall through to the plain createTask path.
    expect(calls.createTask).toHaveLength(0);
    expect(calls.ensureScheduledRun).toHaveLength(1);
    const request = calls.ensureScheduledRun[0];
    if (!request) {
      throw new Error("expected an ensureScheduledRun request");
    }
    expect(request.expectedTaskId).toBe(derivedTaskId);
    expect(request.identity.scheduleWindow.startMs).toBe(window.startMs);
    expect(request.identity.mailboxScope).toEqual(mailboxScope);
    expect(response.statusCode).toBe(201);
    expect(response.body).toEqual({ status: "created", task: current });
  });

  it("rejects a client-supplied scheduled task id that does not match the derived identity", async () => {
    const calls: FakeServiceCalls = { createTask: [], ensureScheduledRun: [] };
    const hub = new RecordingHub();
    const response = new CapturingResponse();
    const service = fakeService({
      calls,
      ensure: (request) =>
        request.expectedTaskId !== undefined && request.expectedTaskId !== derivedTaskId
          ? Effect.fail(
              new SessionServicePersistenceError(
                "ensureScheduledRun",
                new ScheduledTaskIdentityMismatchError({
                  derivedTaskId,
                  suppliedTaskId: request.expectedTaskId,
                }),
              ),
            )
          : Effect.die("expected a mismatched task id"),
    });

    await runCreate({
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
        taskId: "task_client_supplied_wrong",
      },
      hub: hub as unknown as SubscriptionHub,
      response,
      service,
    });

    expect(calls.createTask).toHaveLength(0);
    expect(response.statusCode).toBe(409);
    expect(response.body).toMatchObject({
      derivedTaskId,
      reason: "scheduled_task_identity_mismatch",
      suppliedTaskId: "task_client_supplied_wrong",
    });
  });
});
